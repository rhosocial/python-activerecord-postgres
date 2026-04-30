# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_trgm_integration.py
"""
Integration tests for PostgreSQL pg_trgm extension with real database.

These tests require a live PostgreSQL connection with pg_trgm extension installed
and test:
- similarity function and % operator
- GIN index creation for trigram matching
- show_trgm function
- word_similarity function

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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.expression import RawSQLExpression
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# Table names for sync tests
TABLE_TRGM = "test_trgm"
TABLE_TRGM_OPS = "test_trgm_ops"
TABLE_TRGM_IDX = "test_trgm_idx"
TABLE_TRGM_WORD = "test_trgm_word"

# Table names for async tests
TABLE_TRGM_ASYNC = "test_trgm_async"
TABLE_TRGM_OPS_ASYNC = "test_trgm_ops_async"
TABLE_TRGM_IDX_ASYNC = "test_trgm_idx_async"
TABLE_TRGM_WORD_ASYNC = "test_trgm_word_async"


def _setup_trgm_table(backend, dialect, table_name):
    """Create and populate the test_trgm table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    rows = [
        [Literal(dialect, "hello world")],
        [Literal(dialect, "hello earth")],
        [Literal(dialect, "hi there")],
        [Literal(dialect, "completely different")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_trgm_ops_table(backend, dialect, table_name):
    """Create and populate the test_trgm_ops table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    rows = [
        [Literal(dialect, "PostgreSQL database")],
        [Literal(dialect, "MySQL database")],
        [Literal(dialect, "completely unrelated")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_trgm_idx_table(backend, dialect, table_name):
    """Create test_trgm_idx table, GIN index with gin_trgm_ops, and insert data.

    Note: GIN index with gin_trgm_ops opclass is created via raw SQL because
    the expression system lacks structural support for operator class (opclass)
    specification in CREATE INDEX statements.
    """
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Create GIN index with gin_trgm_ops opclass via raw SQL
    # (expression system lacks structural support for opclass specification)
    backend.execute(
        f"CREATE INDEX IF NOT EXISTS idx_trgm_name ON {table_name} USING gin (name gin_trgm_ops)"
    )

    rows = [
        [Literal(dialect, "application development")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_trgm_word_table(backend, dialect, table_name):
    """Create and populate the test_trgm_word table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="description", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    rows = [
        [Literal(dialect, "The quick brown fox jumps over the lazy dog")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["description"],
        source=ValuesSource(dialect, rows),
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


async def _async_setup_trgm_table(backend, dialect, table_name):
    """Async: create and populate the test_trgm table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    rows = [
        [Literal(dialect, "hello world")],
        [Literal(dialect, "hello earth")],
        [Literal(dialect, "hi there")],
        [Literal(dialect, "completely different")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_trgm_ops_table(backend, dialect, table_name):
    """Async: create and populate the test_trgm_ops table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    rows = [
        [Literal(dialect, "PostgreSQL database")],
        [Literal(dialect, "MySQL database")],
        [Literal(dialect, "completely unrelated")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_trgm_idx_table(backend, dialect, table_name):
    """Async: create test_trgm_idx table, GIN index with gin_trgm_ops, and insert data.

    Note: GIN index with gin_trgm_ops opclass is created via raw SQL because
    the expression system lacks structural support for operator class (opclass)
    specification in CREATE INDEX statements.
    """
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Create GIN index with gin_trgm_ops opclass via raw SQL
    # (expression system lacks structural support for opclass specification)
    await backend.execute(
        f"CREATE INDEX IF NOT EXISTS idx_trgm_name_async ON {table_name} USING gin (name gin_trgm_ops)"
    )

    rows = [
        [Literal(dialect, "application development")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_trgm_word_table(backend, dialect, table_name):
    """Async: create and populate the test_trgm_word table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="description", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    rows = [
        [Literal(dialect, "The quick brown fox jumps over the lazy dog")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["description"],
        source=ValuesSource(dialect, rows),
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


# --- Sync fixture and tests ---


@pytest.fixture
def pg_trgm_env(postgres_backend_single):
    """Independent test environment for pg_trgm extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_trgm")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_TRGM, TABLE_TRGM_OPS, TABLE_TRGM_IDX, TABLE_TRGM_WORD]:
        _teardown_table(backend, dialect, table_name)

    _setup_trgm_table(backend, dialect, TABLE_TRGM)
    _setup_trgm_ops_table(backend, dialect, TABLE_TRGM_OPS)
    _setup_trgm_idx_table(backend, dialect, TABLE_TRGM_IDX)
    _setup_trgm_word_table(backend, dialect, TABLE_TRGM_WORD)

    yield backend, dialect

    _teardown_table(backend, dialect, TABLE_TRGM)
    _teardown_table(backend, dialect, TABLE_TRGM_OPS)
    _teardown_table(backend, dialect, TABLE_TRGM_IDX)
    _teardown_table(backend, dialect, TABLE_TRGM_WORD)


class TestPgTrgmIntegration:
    """Integration tests for pg_trgm extension."""

    def test_trgm_similarity(self, pg_trgm_env):
        """Test similarity function with text data."""
        backend, dialect = pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test similarity function for "hello world" (id=1)
        sim_func = FunctionCall(
            dialect, "similarity",
            Literal(dialect, "hello"),
            Column(dialect, "name"),
        ).as_("sim")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[sim_func],
            from_=TableExpression(dialect, TABLE_TRGM),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["sim"] is not None
        assert result.data[0]["sim"] > 0.0

        # "hello world" should be more similar to "hello" than "completely different"
        # Query for id=1 ("hello world")
        sim_func_world = FunctionCall(
            dialect, "similarity",
            Literal(dialect, "hello"),
            Column(dialect, "name"),
        ).as_("sim")
        where_pred_world = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query_world = QueryExpression(
            dialect=dialect,
            select=[sim_func_world],
            from_=TableExpression(dialect, TABLE_TRGM),
            where=where_pred_world,
        )
        sql, params = query_world.to_sql()
        result_world = backend.execute(sql, params, options=opts)

        # Query for id=4 ("completely different")
        sim_func_diff = FunctionCall(
            dialect, "similarity",
            Literal(dialect, "hello"),
            Column(dialect, "name"),
        ).as_("sim")
        where_pred_diff = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 4),
        )
        query_diff = QueryExpression(
            dialect=dialect,
            select=[sim_func_diff],
            from_=TableExpression(dialect, TABLE_TRGM),
            where=where_pred_diff,
        )
        sql, params = query_diff.to_sql()
        result_different = backend.execute(sql, params, options=opts)

        assert result_world.data[0]["sim"] > result_different.data[0]["sim"]

    def test_trgm_similarity_operator(self, pg_trgm_env):
        """Test % similarity operator for text matching via similarity() threshold."""
        backend, dialect = pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # The % operator checks if similarity >= current threshold.
        # We use similarity() function with threshold comparison as the expression
        # equivalent, since % is a pg_trgm-specific operator with no direct
        # function equivalent in the expression system.
        sim_func_select = FunctionCall(
            dialect, "similarity",
            Column(dialect, "name"),
            Literal(dialect, "PostgreSQL"),
        ).as_("sim")
        # Use a separate FunctionCall without alias in WHERE (aliases invalid in WHERE)
        sim_func_where = FunctionCall(
            dialect, "similarity",
            Column(dialect, "name"),
            Literal(dialect, "PostgreSQL"),
        )
        where_pred = ComparisonPredicate(
            dialect, ">=",
            sim_func_where,
            Literal(dialect, 0.3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name"), sim_func_select],
            from_=TableExpression(dialect, TABLE_TRGM_OPS),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) >= 1
        # PostgreSQL database should be in the results
        names = [row["name"] for row in result.data]
        assert "PostgreSQL database" in names

    def test_trgm_index(self, pg_trgm_env):
        """Test creating GIN index with pg_trgm operator class."""
        backend, dialect = pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify index exists via pg_indexes
        where_tablename = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "tablename"),
            Literal(dialect, TABLE_TRGM_IDX),
        )
        where_indexname = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "indexname"),
            Literal(dialect, "idx_trgm_name"),
        )
        # Combine both conditions with AND
        combined_pred = ComparisonPredicate(
            dialect, "AND",
            where_tablename,
            where_indexname,
        )
        # TODO: COUNT(*) requires RawSQLExpression because Literal("*")
        # gets parameterized as $1 and PostgreSQL can't determine its type.
        # Replace with FunctionCall once expression system supports star args.
        count_func = RawSQLExpression(
            dialect, 'COUNT(*) AS "cnt"'
        )
        query = QueryExpression(
            dialect=dialect,
            select=[count_func],
            from_=TableExpression(dialect, "pg_indexes"),
            where=combined_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["cnt"] >= 1

        # Query using the index via similarity() threshold (equivalent to name % 'application')
        sim_func = FunctionCall(
            dialect, "similarity",
            Column(dialect, "name"),
            Literal(dialect, "application"),
        )
        where_pred = ComparisonPredicate(
            dialect, ">=",
            sim_func,
            Literal(dialect, 0.3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, TABLE_TRGM_IDX),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert len(result.data) >= 1

    def test_trgm_show_trgm(self, pg_trgm_env):
        """Test show_trgm function to extract trigrams."""
        backend, dialect = pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = FunctionCall(
            dialect, "show_trgm",
            Literal(dialect, "hello"),
        ).as_("trigrams")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["trigrams"] is not None
        # show_trgm returns an array of trigrams
        assert len(result.data[0]["trigrams"]) > 0

    def test_trgm_word_similarity(self, pg_trgm_env):
        """Test word_similarity function (PG 11+)."""
        backend, dialect = pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test word_similarity function
        ws_func = FunctionCall(
            dialect, "word_similarity",
            Literal(dialect, "fox"),
            Column(dialect, "description"),
        ).as_("ws")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[ws_func],
            from_=TableExpression(dialect, TABLE_TRGM_WORD),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["ws"] is not None
        assert result.data[0]["ws"] > 0.0


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_pg_trgm_env(async_postgres_backend_single):
    """Independent async test environment for pg_trgm extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_trgm")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_TRGM_ASYNC, TABLE_TRGM_OPS_ASYNC, TABLE_TRGM_IDX_ASYNC, TABLE_TRGM_WORD_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)

    await _async_setup_trgm_table(backend, dialect, TABLE_TRGM_ASYNC)
    await _async_setup_trgm_ops_table(backend, dialect, TABLE_TRGM_OPS_ASYNC)
    await _async_setup_trgm_idx_table(backend, dialect, TABLE_TRGM_IDX_ASYNC)
    await _async_setup_trgm_word_table(backend, dialect, TABLE_TRGM_WORD_ASYNC)

    yield backend, dialect

    await _async_teardown_table(backend, dialect, TABLE_TRGM_ASYNC)
    await _async_teardown_table(backend, dialect, TABLE_TRGM_OPS_ASYNC)
    await _async_teardown_table(backend, dialect, TABLE_TRGM_IDX_ASYNC)
    await _async_teardown_table(backend, dialect, TABLE_TRGM_WORD_ASYNC)


class TestAsyncPgTrgmIntegration:
    """Async integration tests for pg_trgm extension."""

    @pytest.mark.asyncio
    async def test_async_trgm_similarity(self, async_pg_trgm_env):
        """Test similarity function with text data."""
        backend, dialect = async_pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test similarity function for "hello world" (id=1)
        sim_func = FunctionCall(
            dialect, "similarity",
            Literal(dialect, "hello"),
            Column(dialect, "name"),
        ).as_("sim")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[sim_func],
            from_=TableExpression(dialect, TABLE_TRGM_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["sim"] is not None
        assert result.data[0]["sim"] > 0.0

        # "hello world" should be more similar to "hello" than "completely different"
        # Query for id=1 ("hello world")
        sim_func_world = FunctionCall(
            dialect, "similarity",
            Literal(dialect, "hello"),
            Column(dialect, "name"),
        ).as_("sim")
        where_pred_world = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query_world = QueryExpression(
            dialect=dialect,
            select=[sim_func_world],
            from_=TableExpression(dialect, TABLE_TRGM_ASYNC),
            where=where_pred_world,
        )
        sql, params = query_world.to_sql()
        result_world = await backend.execute(sql, params, options=opts)

        # Query for id=4 ("completely different")
        sim_func_diff = FunctionCall(
            dialect, "similarity",
            Literal(dialect, "hello"),
            Column(dialect, "name"),
        ).as_("sim")
        where_pred_diff = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 4),
        )
        query_diff = QueryExpression(
            dialect=dialect,
            select=[sim_func_diff],
            from_=TableExpression(dialect, TABLE_TRGM_ASYNC),
            where=where_pred_diff,
        )
        sql, params = query_diff.to_sql()
        result_different = await backend.execute(sql, params, options=opts)

        assert result_world.data[0]["sim"] > result_different.data[0]["sim"]

    @pytest.mark.asyncio
    async def test_async_trgm_similarity_operator(self, async_pg_trgm_env):
        """Test % similarity operator for text matching via similarity() threshold."""
        backend, dialect = async_pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # The % operator checks if similarity >= current threshold.
        # We use similarity() function with threshold comparison as the expression
        # equivalent, since % is a pg_trgm-specific operator with no direct
        # function equivalent in the expression system.
        sim_func_select = FunctionCall(
            dialect, "similarity",
            Column(dialect, "name"),
            Literal(dialect, "PostgreSQL"),
        ).as_("sim")
        # Use a separate FunctionCall without alias in WHERE (aliases invalid in WHERE)
        sim_func_where = FunctionCall(
            dialect, "similarity",
            Column(dialect, "name"),
            Literal(dialect, "PostgreSQL"),
        )
        where_pred = ComparisonPredicate(
            dialect, ">=",
            sim_func_where,
            Literal(dialect, 0.3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name"), sim_func_select],
            from_=TableExpression(dialect, TABLE_TRGM_OPS_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) >= 1
        # PostgreSQL database should be in the results
        names = [row["name"] for row in result.data]
        assert "PostgreSQL database" in names

    @pytest.mark.asyncio
    async def test_async_trgm_index(self, async_pg_trgm_env):
        """Test creating GIN index with pg_trgm operator class."""
        backend, dialect = async_pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify index exists via pg_indexes
        where_tablename = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "tablename"),
            Literal(dialect, TABLE_TRGM_IDX_ASYNC),
        )
        where_indexname = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "indexname"),
            Literal(dialect, "idx_trgm_name_async"),
        )
        combined_pred = ComparisonPredicate(
            dialect, "AND",
            where_tablename,
            where_indexname,
        )
        # TODO: COUNT(*) requires RawSQLExpression because Literal("*")
        # gets parameterized as $1 and PostgreSQL can't determine its type.
        count_func = RawSQLExpression(
            dialect, 'COUNT(*) AS "cnt"'
        )
        query = QueryExpression(
            dialect=dialect,
            select=[count_func],
            from_=TableExpression(dialect, "pg_indexes"),
            where=combined_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["cnt"] >= 1

        # Query using the index via similarity() threshold (equivalent to name % 'application')
        sim_func = FunctionCall(
            dialect, "similarity",
            Column(dialect, "name"),
            Literal(dialect, "application"),
        )
        where_pred = ComparisonPredicate(
            dialect, ">=",
            sim_func,
            Literal(dialect, 0.3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, TABLE_TRGM_IDX_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert len(result.data) >= 1

    @pytest.mark.asyncio
    async def test_async_trgm_show_trgm(self, async_pg_trgm_env):
        """Test show_trgm function to extract trigrams."""
        backend, dialect = async_pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = FunctionCall(
            dialect, "show_trgm",
            Literal(dialect, "hello"),
        ).as_("trigrams")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["trigrams"] is not None
        # show_trgm returns an array of trigrams
        assert len(result.data[0]["trigrams"]) > 0

    @pytest.mark.asyncio
    async def test_async_trgm_word_similarity(self, async_pg_trgm_env):
        """Test word_similarity function (PG 11+)."""
        backend, dialect = async_pg_trgm_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test word_similarity function
        ws_func = FunctionCall(
            dialect, "word_similarity",
            Literal(dialect, "fox"),
            Column(dialect, "description"),
        ).as_("ws")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[ws_func],
            from_=TableExpression(dialect, TABLE_TRGM_WORD_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["ws"] is not None
        assert result.data[0]["ws"] > 0.0
