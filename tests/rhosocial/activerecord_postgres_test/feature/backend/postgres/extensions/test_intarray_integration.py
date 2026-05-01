# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_intarray_integration.py

"""Integration tests for PostgreSQL intarray extension with real database.

These tests require a live PostgreSQL connection with intarray extension installed
and test:
- @> (contains) operator for integer arrays
- && (overlap) operator for integer arrays
- <@ (contained by) operator for integer arrays
- idx() function for array element access
- GIN index with gin__int_ops operator class

All database operations use expression objects, not raw SQL strings.
Exception: CREATE INDEX with opclass (gin__int_ops) uses raw SQL since
the expression system does not yet support opclass specifications.
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
    OrderByClause,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall, Subquery
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# --- Helper functions ---


def _setup_intarray_table(backend, dialect, table_name):
    """Create and populate the basic intarray test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "{1,2,3,4,5}").cast("integer[]")],
                [Literal(dialect, "{10,20,30}").cast("integer[]")],
                [Literal(dialect, "{3,4}").cast("integer[]")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_intarray_overlap_table(backend, dialect, table_name):
    """Create and populate the intarray overlap test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "{1,2,3}").cast("integer[]")],
                [Literal(dialect, "{4,5,6}").cast("integer[]")],
                [Literal(dialect, "{3,4}").cast("integer[]")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_intarray_contained_table(backend, dialect, table_name):
    """Create and populate the intarray contained-by test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "{1,2}").cast("integer[]")],
                [Literal(dialect, "{1,2,3,4,5}").cast("integer[]")],
                [Literal(dialect, "{10,20}").cast("integer[]")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_intarray_idx_table(backend, dialect, table_name):
    """Create and populate the intarray idx() test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "{10,20,30,40}").cast("integer[]")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_intarray_gin_table(backend, dialect, table_name, index_name):
    """Create and populate the intarray GIN test table using expressions.

    The GIN index with gin__int_ops opclass is created using raw SQL
    because the expression system does not yet support opclass specifications.
    """
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # GIN index with gin__int_ops opclass requires raw SQL
    # (expression system does not support opclass specifications)
    backend.execute(
        f"CREATE INDEX IF NOT EXISTS {index_name} "
        f"ON {table_name} USING gin (tags gin__int_ops)"
    )

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "{1,5,10}").cast("integer[]")]],
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


async def _async_setup_intarray_table(backend, dialect, table_name):
    """Async: create and populate the basic intarray test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "{1,2,3,4,5}").cast("integer[]")],
                [Literal(dialect, "{10,20,30}").cast("integer[]")],
                [Literal(dialect, "{3,4}").cast("integer[]")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_intarray_overlap_table(backend, dialect, table_name):
    """Async: create and populate the intarray overlap test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "{1,2,3}").cast("integer[]")],
                [Literal(dialect, "{4,5,6}").cast("integer[]")],
                [Literal(dialect, "{3,4}").cast("integer[]")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_intarray_contained_table(backend, dialect, table_name):
    """Async: create and populate the intarray contained-by test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "{1,2}").cast("integer[]")],
                [Literal(dialect, "{1,2,3,4,5}").cast("integer[]")],
                [Literal(dialect, "{10,20}").cast("integer[]")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_intarray_idx_table(backend, dialect, table_name):
    """Async: create and populate the intarray idx() test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
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
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "{10,20,30,40}").cast("integer[]")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_intarray_gin_table(backend, dialect, table_name, index_name):
    """Async: create and populate the intarray GIN test table using expressions.

    The GIN index with gin__int_ops opclass is created using raw SQL
    because the expression system does not yet support opclass specifications.
    """
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="tags", data_type="INTEGER[]"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # GIN index with gin__int_ops opclass requires raw SQL
    await backend.execute(
        f"CREATE INDEX IF NOT EXISTS {index_name} "
        f"ON {table_name} USING gin (tags gin__int_ops)"
    )

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["tags"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "{1,5,10}").cast("integer[]")]],
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

T_INTARRAY = "test_intarray"
T_INTARRAY_OVERLAP = "test_intarray_overlap"
T_INTARRAY_CONTAINED = "test_intarray_contained"
T_INTARRAY_IDX = "test_intarray_idx"
T_INTARRAY_GIN = "test_intarray_gin"
IDX_GIN_NAME = "idx_intarray_tags"

T_INTARRAY_ASYNC = "test_intarray_async"
T_INTARRAY_OVERLAP_ASYNC = "test_intarray_overlap_async"
T_INTARRAY_CONTAINED_ASYNC = "test_intarray_contained_async"
T_INTARRAY_IDX_ASYNC = "test_intarray_idx_async"
T_INTARRAY_GIN_ASYNC = "test_intarray_gin_async"
IDX_GIN_NAME_ASYNC = "idx_intarray_tags_async"


# --- Sync fixture and tests ---


@pytest.fixture
def intarray_env(postgres_backend_single):
    """Independent test environment for intarray extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "intarray")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_INTARRAY, T_INTARRAY_OVERLAP, T_INTARRAY_CONTAINED,
                       T_INTARRAY_IDX, T_INTARRAY_GIN]:
        _teardown_table(backend, dialect, table_name)

    _setup_intarray_table(backend, dialect, T_INTARRAY)
    _setup_intarray_overlap_table(backend, dialect, T_INTARRAY_OVERLAP)
    _setup_intarray_contained_table(backend, dialect, T_INTARRAY_CONTAINED)
    _setup_intarray_idx_table(backend, dialect, T_INTARRAY_IDX)
    _setup_intarray_gin_table(backend, dialect, T_INTARRAY_GIN, IDX_GIN_NAME)

    yield backend, dialect

    for table_name in [T_INTARRAY, T_INTARRAY_OVERLAP, T_INTARRAY_CONTAINED,
                       T_INTARRAY_IDX, T_INTARRAY_GIN]:
        _teardown_table(backend, dialect, table_name)


class TestIntarrayIntegration:
    """Integration tests for intarray extension."""

    def test_intarray_contains_operator(self, intarray_env):
        """Test @> (contains) operator for integer arrays."""
        backend, dialect = intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # SELECT id, tags @> '{3,4}'::integer[] AS contains FROM test_intarray
        contains_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "@>",
                Column(dialect, "tags"),
                Literal(dialect, "{3,4}").cast("integer[]"),
            ),
        ).as_("contains")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr],
            from_=TableExpression(dialect, T_INTARRAY),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None

        ids = [r['id'] for r in result.data if r['contains'] is True]
        assert 1 in ids  # {1,2,3,4,5} contains {3,4}
        assert 3 in ids  # {3,4} contains {3,4}
        assert 2 not in ids  # {10,20,30} does NOT contain {3,4}

    def test_intarray_overlap_operator(self, intarray_env):
        """Test && (overlap) operator for integer arrays."""
        backend, dialect = intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # SELECT id, tags && '{3,4}'::integer[] AS overlaps FROM test_intarray_overlap
        overlap_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "&&",
                Column(dialect, "tags"),
                Literal(dialect, "{3,4}").cast("integer[]"),
            ),
        ).as_("overlaps")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), overlap_expr],
            from_=TableExpression(dialect, T_INTARRAY_OVERLAP),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None

        ids = [r['id'] for r in result.data if r['overlaps'] is True]
        assert 1 in ids  # {1,2,3} overlaps with {3,4}
        assert 2 in ids  # {4,5,6} overlaps with {3,4}
        assert 3 in ids  # {3,4} overlaps with {3,4}

    def test_intarray_contained_by_operator(self, intarray_env):
        """Test <@ (contained by) operator for integer arrays."""
        backend, dialect = intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # SELECT id, tags <@ '{1,2,3,4,5}'::integer[] AS contained
        # FROM test_intarray_contained
        contained_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "<@",
                Column(dialect, "tags"),
                Literal(dialect, "{1,2,3,4,5}").cast("integer[]"),
            ),
        ).as_("contained")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contained_expr],
            from_=TableExpression(dialect, T_INTARRAY_CONTAINED),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None

        ids = [r['id'] for r in result.data if r['contained'] is True]
        assert 1 in ids  # {1,2} is contained by {1,2,3,4,5}
        assert 2 in ids  # {1,2,3,4,5} is contained by itself
        assert 3 not in ids  # {10,20} is NOT contained by {1,2,3,4,5}

    def test_intarray_idx_function(self, intarray_env):
        """Test idx() function for finding element position in array."""
        backend, dialect = intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # idx() returns the position of an element (1-based)
        # SELECT idx(tags, 20) AS pos FROM test_intarray_idx WHERE id = 1
        idx_func = FunctionCall(
            dialect, "idx",
            Column(dialect, "tags"),
            Literal(dialect, 20),
        ).as_("pos")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[idx_func],
            from_=TableExpression(dialect, T_INTARRAY_IDX),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['pos'] == 2

        # Element not found returns 0
        idx_func2 = FunctionCall(
            dialect, "idx",
            Column(dialect, "tags"),
            Literal(dialect, 99),
        ).as_("pos")
        query2 = QueryExpression(
            dialect=dialect,
            select=[idx_func2],
            from_=TableExpression(dialect, T_INTARRAY_IDX),
            where=where_pred,
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['pos'] == 0

    def test_intarray_gin_index(self, intarray_env):
        """Test creating GIN index on integer array with intarray operator class."""
        backend, dialect = intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify index exists via pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "tablename"),
                Literal(dialect, T_INTARRAY_GIN),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [r['indexname'] for r in result.data]
        assert IDX_GIN_NAME in index_names

        # Query using the GIN index with @> operator via Literal
        contains_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "@>",
                Column(dialect, "tags"),
                Literal(dialect, "{5}").cast("integer[]"),
            ),
        ).as_("contains")
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr],
            from_=TableExpression(dialect, T_INTARRAY_GIN),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        matching = [r for r in result2.data if r['contains'] is True]
        assert len(matching) >= 1


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_intarray_env(async_postgres_backend_single):
    """Independent async test environment for intarray extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "intarray")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_INTARRAY_ASYNC, T_INTARRAY_OVERLAP_ASYNC,
                       T_INTARRAY_CONTAINED_ASYNC, T_INTARRAY_IDX_ASYNC,
                       T_INTARRAY_GIN_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)

    await _async_setup_intarray_table(backend, dialect, T_INTARRAY_ASYNC)
    await _async_setup_intarray_overlap_table(backend, dialect, T_INTARRAY_OVERLAP_ASYNC)
    await _async_setup_intarray_contained_table(backend, dialect, T_INTARRAY_CONTAINED_ASYNC)
    await _async_setup_intarray_idx_table(backend, dialect, T_INTARRAY_IDX_ASYNC)
    await _async_setup_intarray_gin_table(backend, dialect, T_INTARRAY_GIN_ASYNC, IDX_GIN_NAME_ASYNC)

    yield backend, dialect

    for table_name in [T_INTARRAY_ASYNC, T_INTARRAY_OVERLAP_ASYNC,
                       T_INTARRAY_CONTAINED_ASYNC, T_INTARRAY_IDX_ASYNC,
                       T_INTARRAY_GIN_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)


class TestAsyncIntarrayIntegration:
    """Async integration tests for intarray extension."""

    @pytest.mark.asyncio
    async def test_async_intarray_contains_operator(self, async_intarray_env):
        """Test @> (contains) operator for integer arrays."""
        backend, dialect = async_intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        contains_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "@>",
                Column(dialect, "tags"),
                Literal(dialect, "{3,4}").cast("integer[]"),
            ),
        ).as_("contains")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr],
            from_=TableExpression(dialect, T_INTARRAY_ASYNC),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None

        ids = [r['id'] for r in result.data if r['contains'] is True]
        assert 1 in ids  # {1,2,3,4,5} contains {3,4}
        assert 3 in ids  # {3,4} contains {3,4}
        assert 2 not in ids  # {10,20,30} does NOT contain {3,4}

    @pytest.mark.asyncio
    async def test_async_intarray_overlap_operator(self, async_intarray_env):
        """Test && (overlap) operator for integer arrays."""
        backend, dialect = async_intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        overlap_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "&&",
                Column(dialect, "tags"),
                Literal(dialect, "{3,4}").cast("integer[]"),
            ),
        ).as_("overlaps")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), overlap_expr],
            from_=TableExpression(dialect, T_INTARRAY_OVERLAP_ASYNC),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None

        ids = [r['id'] for r in result.data if r['overlaps'] is True]
        assert 1 in ids  # {1,2,3} overlaps with {3,4}
        assert 2 in ids  # {4,5,6} overlaps with {3,4}
        assert 3 in ids  # {3,4} overlaps with {3,4}

    @pytest.mark.asyncio
    async def test_async_intarray_contained_by_operator(self, async_intarray_env):
        """Test <@ (contained by) operator for integer arrays."""
        backend, dialect = async_intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        contained_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "<@",
                Column(dialect, "tags"),
                Literal(dialect, "{1,2,3,4,5}").cast("integer[]"),
            ),
        ).as_("contained")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contained_expr],
            from_=TableExpression(dialect, T_INTARRAY_CONTAINED_ASYNC),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None

        ids = [r['id'] for r in result.data if r['contained'] is True]
        assert 1 in ids  # {1,2} is contained by {1,2,3,4,5}
        assert 2 in ids  # {1,2,3,4,5} is contained by itself
        assert 3 not in ids  # {10,20} is NOT contained by {1,2,3,4,5}

    @pytest.mark.asyncio
    async def test_async_intarray_idx_function(self, async_intarray_env):
        """Test idx() function for finding element position in array."""
        backend, dialect = async_intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # idx() returns the position of an element (1-based)
        idx_func = FunctionCall(
            dialect, "idx",
            Column(dialect, "tags"),
            Literal(dialect, 20),
        ).as_("pos")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[idx_func],
            from_=TableExpression(dialect, T_INTARRAY_IDX_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['pos'] == 2

        # Element not found returns 0
        idx_func2 = FunctionCall(
            dialect, "idx",
            Column(dialect, "tags"),
            Literal(dialect, 99),
        ).as_("pos")
        query2 = QueryExpression(
            dialect=dialect,
            select=[idx_func2],
            from_=TableExpression(dialect, T_INTARRAY_IDX_ASYNC),
            where=where_pred,
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['pos'] == 0

    @pytest.mark.asyncio
    async def test_async_intarray_gin_index(self, async_intarray_env):
        """Test creating GIN index on integer array with intarray operator class."""
        backend, dialect = async_intarray_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify index exists via pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "tablename"),
                Literal(dialect, T_INTARRAY_GIN_ASYNC),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [r['indexname'] for r in result.data]
        assert IDX_GIN_NAME_ASYNC in index_names

        # Query using the GIN index with @> operator via Literal
        contains_expr = Subquery(
            dialect,
            ComparisonPredicate(
                dialect, "@>",
                Column(dialect, "tags"),
                Literal(dialect, "{5}").cast("integer[]"),
            ),
        ).as_("contains")
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr],
            from_=TableExpression(dialect, T_INTARRAY_GIN_ASYNC),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        matching = [r for r in result2.data if r['contains'] is True]
        assert len(matching) >= 1
