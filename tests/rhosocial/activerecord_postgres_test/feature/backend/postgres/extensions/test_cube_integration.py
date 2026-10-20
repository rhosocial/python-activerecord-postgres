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
    BinaryExpression,
    CreateTableStatement,
    ColumnDefinition,
    ColumnConstraint,
    DropTableStatement,
    InsertStatement,
    QueryStatement,
    Column,
    CreateIndexStatement,
    OrderByClause,
    ColumnPrimaryKeyConstraint,
)
from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall, Subquery
from rhosocial.activerecord.backend.expression import cast
from rhosocial.activerecord.backend.expression import RawSQL
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresCubeType,
    PostgresSerialType,
)


# --- Sync fixture and tests ---


@pytest.fixture
def cube_env(postgres_backend_single):
    """Independent test environment for cube extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "cube")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table in ["test_cubes", "test_cube_dist", "test_cube_idx"]:
        drop_expr = DropTableStatement(dialect=dialect, table=Table(dialect, table), if_exists=True)
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)

    # Setup: create test_cubes table
    create_cubes = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'test_cubes'),
        columns=[
            ColumnDefinition(
                dialect, name="id",
                data_type=PostgresSerialType(dialect=dialect),
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                ],
            ),
            ColumnDefinition(dialect, name="c", data_type=PostgresCubeType(dialect=dialect)),
        ],
        if_not_exists=True,
    )
    sql, params = create_cubes.to_sql()
    backend.execute(sql, params)

    # Insert data into test_cubes
    insert_cubes = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'test_cubes'),
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [cast(dialect, Literal(dialect, "(1,2,3)"), PostgresCubeType(dialect))],
                [cast(dialect, Literal(dialect, "(4,5,6)"), PostgresCubeType(dialect))],
            ],
        ),
    )
    sql, params = insert_cubes.to_sql()
    backend.execute(sql, params)

    # Setup: create test_cube_dist table
    create_dist = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'test_cube_dist'),
        columns=[
            ColumnDefinition(
                dialect, name="id",
                data_type=PostgresSerialType(dialect=dialect),
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                ],
            ),
            ColumnDefinition(dialect, name="c", data_type=PostgresCubeType(dialect=dialect)),
        ],
        if_not_exists=True,
    )
    sql, params = create_dist.to_sql()
    backend.execute(sql, params)

    # Insert data into test_cube_dist
    insert_dist = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'test_cube_dist'),
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [cast(dialect, Literal(dialect, "(1,1)"), PostgresCubeType(dialect))],
                [cast(dialect, Literal(dialect, "(4,5)"), PostgresCubeType(dialect))],
            ],
        ),
    )
    sql, params = insert_dist.to_sql()
    backend.execute(sql, params)

    # Setup: create test_cube_idx table
    create_idx_table = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'test_cube_idx'),
        columns=[
            ColumnDefinition(
                dialect, name="id",
                data_type=PostgresSerialType(dialect=dialect),
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                ],
            ),
            ColumnDefinition(dialect, name="c", data_type=PostgresCubeType(dialect=dialect)),
        ],
        if_not_exists=True,
    )
    sql, params = create_idx_table.to_sql()
    backend.execute(sql, params)

    # Insert data into test_cube_idx
    insert_idx = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'test_cube_idx'),
        columns=["c"],
        source=ValuesSource(
            dialect,
            [[cast(dialect, Literal(dialect, "(1,2,3)"), PostgresCubeType(dialect))]],
        ),
    )
    sql, params = insert_idx.to_sql()
    backend.execute(sql, params)

    # Create GiST index on test_cube_idx
    create_index = CreateIndexStatement(
        dialect=dialect,
        index=Index(dialect, 'idx_test_cube'),
        table=Table(dialect, 'test_cube_idx'),
        columns=["c"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table in ["test_cubes", "test_cube_dist", "test_cube_idx"]:
        drop_expr = DropTableStatement(
            dialect=dialect,
            table=Table(dialect, table),
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

        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "c")],
            from_=Table(dialect, "test_cubes"),
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
            cast(dialect, Literal(dialect, "(1,1)"), PostgresCubeType(dialect)),
            cast(dialect, Literal(dialect, "(4,5)"), PostgresCubeType(dialect)),
        ).as_("distance")
        query = QueryStatement(
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
                cast(dialect, Literal(dialect, "(0,0),(10,10)"), PostgresCubeType(dialect)),
                cast(dialect, Literal(dialect, "(5,5)"), PostgresCubeType(dialect)),
            ),
        ).as_("contains")
        query = QueryStatement(
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
                cast(dialect, Literal(dialect, "(0,0),(10,10)"), PostgresCubeType(dialect)),
                cast(dialect, Literal(dialect, "(15,15)"), PostgresCubeType(dialect)),
            ),
        ).as_("contains")
        query2 = QueryStatement(
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
            BinaryExpression(
            dialect, '&&',
                cast(dialect, Literal(dialect, "(0,0),(5,5)"), PostgresCubeType(dialect)),
                cast(dialect, Literal(dialect, "(3,3),(8,8)"), PostgresCubeType(dialect)),
            ),
        ).as_("overlaps")
        query = QueryStatement(
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
            BinaryExpression(
            dialect, '&&',
                cast(dialect, Literal(dialect, "(0,0),(2,2)"), PostgresCubeType(dialect)),
                cast(dialect, Literal(dialect, "(5,5),(8,8)"), PostgresCubeType(dialect)),
            ),
        ).as_("overlaps")
        query2 = QueryStatement(
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

        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=Table(dialect, "pg_indexes"),
            where=ComparisonPredicate(dialect, "=", Column(dialect, "tablename"), Literal(dialect, "test_cube_idx")),
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
    for table in ["test_cubes_async", "test_cube_dist_async", "test_cube_idx_async"]:
        drop_expr = DropTableStatement(dialect=dialect, table=Table(dialect, table), if_exists=True)
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)

    # Setup: create test_cubes_async table
    create_cubes = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'test_cubes_async'),
        columns=[
            ColumnDefinition(
                dialect, name="id",
                data_type=PostgresSerialType(dialect=dialect),
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                ],
            ),
            ColumnDefinition(dialect, name="c", data_type=PostgresCubeType(dialect=dialect)),
        ],
        if_not_exists=True,
    )
    sql, params = create_cubes.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_cubes_async
    insert_cubes = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'test_cubes_async'),
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [cast(dialect, Literal(dialect, "(1,2,3)"), PostgresCubeType(dialect))],
                [cast(dialect, Literal(dialect, "(4,5,6)"), PostgresCubeType(dialect))],
            ],
        ),
    )
    sql, params = insert_cubes.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_cube_dist_async table
    create_dist = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'test_cube_dist_async'),
        columns=[
            ColumnDefinition(
                dialect, name="id",
                data_type=PostgresSerialType(dialect=dialect),
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                ],
            ),
            ColumnDefinition(dialect, name="c", data_type=PostgresCubeType(dialect=dialect)),
        ],
        if_not_exists=True,
    )
    sql, params = create_dist.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_cube_dist_async
    insert_dist = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'test_cube_dist_async'),
        columns=["c"],
        source=ValuesSource(
            dialect,
            [
                [cast(dialect, Literal(dialect, "(1,1)"), PostgresCubeType(dialect))],
                [cast(dialect, Literal(dialect, "(4,5)"), PostgresCubeType(dialect))],
            ],
        ),
    )
    sql, params = insert_dist.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_cube_idx_async table
    create_idx_table = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'test_cube_idx_async'),
        columns=[
            ColumnDefinition(
                dialect, name="id",
                data_type=PostgresSerialType(dialect=dialect),
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                ],
            ),
            ColumnDefinition(dialect, name="c", data_type=PostgresCubeType(dialect=dialect)),
        ],
        if_not_exists=True,
    )
    sql, params = create_idx_table.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_cube_idx_async
    insert_idx = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'test_cube_idx_async'),
        columns=["c"],
        source=ValuesSource(
            dialect,
            [[cast(dialect, Literal(dialect, "(1,2,3)"), PostgresCubeType(dialect))]],
        ),
    )
    sql, params = insert_idx.to_sql()
    await backend.execute(sql, params)

    # Create GiST index on test_cube_idx_async
    create_index = CreateIndexStatement(
        dialect=dialect,
        index=Index(dialect, 'idx_test_cube_async'),
        table=Table(dialect, 'test_cube_idx_async'),
        columns=["c"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table in ["test_cubes_async", "test_cube_dist_async", "test_cube_idx_async"]:
        drop_expr = DropTableStatement(
            dialect=dialect,
            table=Table(dialect, table),
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

        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "c")],
            from_=Table(dialect, "test_cubes_async"),
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
            cast(dialect, Literal(dialect, "(1,1)"), PostgresCubeType(dialect)),
            cast(dialect, Literal(dialect, "(4,5)"), PostgresCubeType(dialect)),
        ).as_("distance")
        query = QueryStatement(
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
        contains_expr = RawSQL(
            dialect, "'(0,0),(10,10)'::cube @> '(5,5)'::cube AS contains"
        )
        query = QueryStatement(
            dialect=dialect,
            select=[contains_expr],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['contains'] is True

        # Point outside the range cube
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        not_contains_expr = RawSQL(
            dialect, "'(0,0),(10,10)'::cube @> '(15,15)'::cube AS contains"
        )
        query2 = QueryStatement(
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
        overlap_expr = RawSQL(
            dialect, "'(0,0),(5,5)'::cube && '(3,3),(8,8)'::cube AS overlaps"
        )
        query = QueryStatement(
            dialect=dialect,
            select=[overlap_expr],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['overlaps'] is True

        # Non-overlapping cubes
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        no_overlap_expr = RawSQL(
            dialect, "'(0,0),(2,2)'::cube && '(5,5),(8,8)'::cube AS overlaps"
        )
        query2 = QueryStatement(
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

        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=Table(dialect, "pg_indexes"),
            where=ComparisonPredicate(
                dialect, "=", Column(dialect, "tablename"), Literal(dialect, "test_cube_idx_async")
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [r['indexname'] for r in result.data]
        assert "idx_test_cube_async" in index_names
